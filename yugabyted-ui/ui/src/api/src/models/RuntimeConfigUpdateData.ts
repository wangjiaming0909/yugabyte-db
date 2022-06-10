// tslint:disable
/**
 * Yugabyte Cloud
 * YugabyteDB as a Service
 *
 * The version of the OpenAPI document: v1
 * Contact: support@yugabyte.com
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */




/**
 * Runtime Config data after update
 * @export
 * @interface RuntimeConfigUpdateData
 */
export interface RuntimeConfigUpdateData  {
  /**
   * 
   * @type {string}
   * @memberof RuntimeConfigUpdateData
   */
  config_key: string;
  /**
   * 
   * @type {string}
   * @memberof RuntimeConfigUpdateData
   */
  config_value?: string;
  /**
   * 
   * @type {boolean}
   * @memberof RuntimeConfigUpdateData
   */
  success: boolean;
  /**
   * Success/Failure message
   * @type {string}
   * @memberof RuntimeConfigUpdateData
   */
  error?: string;
}


